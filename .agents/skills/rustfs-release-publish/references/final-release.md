# Final Release Publication

Read only after fresh user confirmation of the preview acceptance report. The [parent confirmation gate](../SKILL.md#human-confirmation) applies.

## Phase 6 — Publish the final tag on the validated commit

No second source version bump. Before creating the final tag, recheck the remote `release/<target>` HEAD equals `PREVIEW_HASH` and the current Console release tag/asset digest still match the accepted preview inputs. If either changed after confirmation, the approval no longer applies; validate the changed inputs with a new preview and obtain fresh confirmation. The final tag goes on the exact commit the preview validated:

```bash
git fetch origin --tags
git rev-parse "<preview-tag>^{commit}"          # must equal PREVIEW_HASH — abort if not
git tag -a "<target>" -m "Release <target>" "$PREVIEW_HASH"
git push origin "<target>"
```

- CI rebuilds from the same source with a different tag name, so the binary self-reports `<target>`. Other inputs can change: `build.yml` reads Console `releases/latest` during each build. Compare the actual Console tag, URL, and verified digest in every final build job with the accepted preview record, and inspect available build provenance for other relevant changes. A pre-tag check cannot prevent a later input change. On a mismatch or missing evidence, repeat affected acceptance against the final artifacts and report the limitation; failed acceptance leaves release validation incomplete even if publication already succeeded. Never move the final tag to conceal a failure.
- Verify the final tag's complete publication path: all matrix and release jobs green; `gh release view "<target>"` shows the full versioned and `-latest` asset set plus checksums, SBOM, and provenance; Docker and Helm workflows succeed; `latest.json` points to `<target>`. A stable target must have `isPrerelease=false` and `isLatest=true`. An alpha/beta/rc target must have `isPrerelease=true`; GitHub does not permit prereleases to be Latest, but the project `latest.json` still advances to the final non-preview target.
- Verify the final Release body contains `## What's Changed` and a Full Changelog link. When `PREVIOUS_DELIVERABLE` exists, the link MUST be `https://github.com/rustfs/rustfs/compare/<PREVIOUS_DELIVERABLE>...<target>` and the baseline MUST equal the preview Release baseline; for example, both `1.0.0-beta.12-preview.1` and `1.0.0-beta.12` compare from `1.0.0-beta.11`.
- Verify the preview cleanup: `cleanup-preview-releases` must succeed and `gh release view "<preview-tag>"` must then report `release not found` for every preview iteration of this target. Each surviving preview tag must still resolve to its own recorded commit; the accepted preview resolves to `PREVIEW_HASH`. If the job failed, delete the leftover Releases manually with `gh release delete "<preview-tag>" --yes` and report it.
- Verify a host-compatible final artifact's checksum and `./rustfs --version`; it must report `<target>` and the expected source SHA. This identity check does not replace affected acceptance when build inputs changed.

Never pass `--cleanup-tag` when removing preview Releases. Remove them manually only if `cleanup-preview-releases` failed, after final publication and notes generation. Record deleted Releases and surviving tags.
