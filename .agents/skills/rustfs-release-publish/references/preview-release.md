# Linux candidate verification

Read for Phase 3 after the source passed exact-SHA CI. The [parent invariants](../SKILL.md) apply; commands and recovery live in the [pipeline runbook](../../../../docs/operations/release-pipeline.md).

Dispatch `release-candidate.yml` on main with the confirmed final version and `CANDIDATE_SHA`. Record the run ID, frozen `inputs.json`, every Linux archive digest and `linux-manifest.json` SHA-256. Require the four Linux builds, native startup/S3 checks, previous-release upgrade matrix and Linux seal to succeed. Missing, skipped, cancelled or failed required work is incomplete acceptance.

Verify the frozen Console identity equals the one accepted in Phase 0. Every matrix job consumes that same digest and exact toolchain. The binary must report the final product version and source commit; its candidate ID is separate. No public tag, Release, registry tag, R2 release path, Helm repository or latest pointer should change during this stage.

A green Linux run is ready for field acceptance, not publication. The partial certification must have `publish_allowed: false`. Retain the exact Linux manifest for the next stage; a new source/build-input candidate must not inherit the old approval. A newer unrelated main commit does not invalidate this evidence.
