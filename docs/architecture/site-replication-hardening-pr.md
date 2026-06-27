## Related Issues

N/A

## Summary of Changes

This PR hardens the site replication control plane in small, reviewable commits. The work starts with guardrails identified during the MinIO comparison and multi-review pass: credential redaction, stricter diagnostic authorization, remove cleanup, and clearer status diagnostics.

### Implementation Log

1. `docs: plan site replication hardening`
   - Purpose: establish the execution plan and PR draft before code changes.
   - Reason: the site replication follow-up touches security, compatibility, operations, and tests, so reviewers need a durable record of each step and why it exists.
2. `fix: redact site replication target secrets`
   - Purpose: prevent bucket replication target credentials from leaking through admin listing, bucket metadata export, and debug formatting.
   - Reason: site replication stores the `site-replicator-0` secret in bucket targets today; keeping the internal storage format intact while redacting external surfaces reduces immediate blast radius without changing replication target loading.
3. `fix: require operation access for replication diagnostics`
   - Purpose: require operation-level admin authorization for site replication diagnostic POST endpoints and clamp netperf duration.
   - Reason: `devnull` and `netperf` read request bodies and exercise diagnostic work, so granting them through read-only site replication info permission was broader than intended.
4. `fix: clean site replication targets on remove`
   - Purpose: remove bucket targets and `site-repl-*` replication rules that point at a removed deployment before completing pending remove.
   - Reason: removing a site only from site replication state leaves bucket-level replication configuration behind, so object replication can continue attempting to reach a removed peer.
5. `feat: expose site replication status diagnostics`
   - Purpose: add machine-readable peer fetch errors and pending operation progress to site replication status.
   - Reason: operators previously saw only `Unknown` sync state or logs when peer metainfo fetches failed or remove/rotation was pending, which made automation and troubleshooting unnecessarily opaque.
6. `docs: expand site replication hardening scope`
   - Purpose: keep the single-PR plan aligned with the expanded request to include MinIO compatibility, add preflight, bootstrap, lifecycle compatibility, retry, and repair work.
   - Reason: the remaining work is larger than the first hardening pass, so reviewers need to see the intended sequence before more behavior changes land.
7. `fix: align site replication peer paths with minio`
   - Purpose: send peer site-replication calls over MinIO-compatible admin paths and accept the MinIO-style peer join route.
   - Reason: RustFS already accepts `/minio/admin` as an alias, so using MinIO wire paths improves mixed-cluster compatibility without removing RustFS route support.
8. `fix: validate site replication add topology`
   - Purpose: preflight add requests with remote metainfo and IDP settings before creating service accounts, joining peers, or persisting state.
   - Reason: MinIO rejects unsafe add topologies up front; RustFS now rejects duplicate deployment IDs, missing local deployment, IDP mismatch, multiple non-empty initial sites, and peers already configured with a different site-replication set.
9. `feat: bootstrap site replication metadata on add`
   - Purpose: replay the local site replication snapshot to joined peers during add, before object backfill starts.
   - Reason: existing hooks only replicate changes after site replication is enabled; existing policies, users, groups, bucket metadata, and buckets must be bootstrapped so a newly joined site does not start with an incomplete control-plane snapshot.
10. `fix: align lifecycle replication with minio semantics`
   - Purpose: replicate bucket lifecycle metadata only when site replication has `replicate-ilm-expiry` enabled.
   - Reason: MinIO keeps lifecycle expiry replication opt-in; RustFS should not replicate lifecycle metadata by default during live bucket metadata hooks or add-time bootstrap.
11. `feat: add site replication retry and repair MVP`
   - Purpose: persist failed peer replication attempts as retry metadata, expose retry counts in status, and add an operation-level repair endpoint that replays the current local snapshot.
   - Reason: transient peer failures were previously only visible through logs and required manual reconstruction; a durable, secret-safe queue plus repair replay gives operators a concrete recovery path without persisting sensitive request payloads.
12. `fix: satisfy site replication pre-pr checks`
   - Purpose: remove a redundant MinIO admin-path branch and construct retry test fixtures directly.
   - Reason: the full pre-PR gate enforces clippy warnings as errors; this keeps the final branch green without changing site replication behavior.

## Verification

Baseline started from `origin/main` at `758677da`, then was rebased onto the latest `origin/main` at `1d6a8259`.

- Passed: `cargo fmt --all --check`
- Passed: `cargo test -p rustfs-ecstore bucket_target --lib`
- Passed: `cargo test -p rustfs-madmin site_replication --lib`
- Passed: `cargo test -p rustfs route_policy --lib`
- Passed: `cargo test -p rustfs site_replication --lib`
- Passed: `cargo test -p rustfs route_policy --lib`
- Passed: `cargo test -p rustfs site_replication --lib`
- Passed: `cargo fmt --all`
- Passed: `cargo fmt --all --check`
- Passed: `cargo test -p rustfs site_replication --lib`
- Passed: `cargo fmt --all --check`
- Passed: `cargo test -p rustfs-madmin site_replication --lib`
- Passed: `cargo test -p rustfs route_policy --lib`
- Passed: `cargo test -p rustfs route_registration_test --lib`
- Passed: `cargo test -p rustfs site_replication --lib`
- Passed: `cargo fmt --all --check`
- Passed: `cargo test -p rustfs site_replication --lib`
- Passed: `cargo fmt --all --check`
- Passed: `cargo test -p rustfs site_replication --lib`
- Passed before the final main rebase: `make pre-pr`
- Not completed after rebasing onto `1d6a8259`: `make pre-pr` was interrupted during clippy at request time; formatting, unsafe-code, architecture, and logging guardrails had already passed in that run.

## Impact

The planned implementation is intended to reduce credential exposure, require stronger permission for diagnostic write-like endpoints, clean removed site replication targets, and make replication status more actionable. Compatibility-sensitive behavior is called out per commit in the implementation log.

The credential redaction step changes admin/export visibility of bucket target secrets. Stored target configuration is not migrated or reformatted by this PR step.

The diagnostic authorization step intentionally changes required permission for `POST /site-replication/devnull` and `POST /site-replication/netperf` from `SiteReplicationInfoAction` to `SiteReplicationOperationAction`.

The remove cleanup step preserves user-managed replication rules and non-replication targets, and only prunes targets/rules associated with removed site replication deployment IDs.

The status diagnostics step adds optional `PeerErrors` and `PendingOperation` fields to the status response. Existing clients can ignore them, while automation can use them to distinguish peer reachability/auth failures from content mismatch.

## Additional Notes

This draft PR includes bootstrap, durable retry, and repair work in one reviewable branch. It should remain draft until CI or a full local pre-PR run confirms the latest default branch baseline.

The expanded single-PR scope also includes MinIO wire-contract bootstrap validation, durable site-replication retry, full add-time IAM/bootstrap sync, lifecycle compatibility, and site-level repair.

The peer path compatibility step maps outbound peer requests to `/minio/admin/v3/site-replication/...`. Peer join uses the encrypted MinIO payload contract, while internal peer metadata/IAM/remove/edit requests remain plain JSON to match MinIO handlers.

The add preflight step performs remote reads before state mutation, so add fails earlier when a peer cannot report metainfo or IDP settings.

The bootstrap step replays IAM policies, built-in users with stored secret material, group membership/status, policy mappings, bucket creation, and bucket metadata through the existing peer replication handlers. It does not synthesize ordinary service-account secrets when the snapshot does not contain them; the site-replicator service account remains distributed through the join flow.

Lifecycle metadata is now skipped by default in site replication hooks and bootstrap snapshots unless the site replication peer state enables `replicate-ilm-expiry`.

The retry MVP intentionally stores peer/path/error/count metadata only. Repair regenerates payloads from the current local snapshot, avoiding persistent storage of user secrets, service-account secrets, or bucket target credentials in the retry queue.
