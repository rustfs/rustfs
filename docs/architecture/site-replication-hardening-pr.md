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

## Verification

Baseline started from `origin/main` at `758677da`.

- Passed: `cargo fmt --all --check`
- Passed: `cargo test -p rustfs-ecstore bucket_target --lib`
- Passed: `cargo test -p rustfs-madmin site_replication --lib`
- Passed: `cargo test -p rustfs route_policy --lib`
- Passed: `cargo test -p rustfs site_replication --lib`
- Passed: `cargo test -p rustfs route_policy --lib`
- Passed: `cargo test -p rustfs site_replication --lib`
- Passed: `cargo fmt --all`

Not run in this step: `make pre-commit`, `make pre-pr`.

## Impact

The planned implementation is intended to reduce credential exposure, require stronger permission for diagnostic write-like endpoints, clean removed site replication targets, and make replication status more actionable. Compatibility-sensitive behavior will be called out per commit in the implementation log.

The credential redaction step changes admin/export visibility of bucket target secrets. Stored target configuration is not migrated or reformatted by this PR step.

The diagnostic authorization step intentionally changes required permission for `POST /site-replication/devnull` and `POST /site-replication/netperf` from `SiteReplicationInfoAction` to `SiteReplicationOperationAction`.

The remove cleanup step preserves user-managed replication rules and non-replication targets, and only prunes targets/rules associated with removed site replication deployment IDs.

The status diagnostics step adds optional `PeerErrors` and `PendingOperation` fields to the status response. Existing clients can ignore them, while automation can use them to distinguish peer reachability/auth failures from content mismatch.

## Additional Notes

This PR intentionally keeps durable outbox/bootstrap/heal work as follow-up architecture unless the initial hardening steps require small supporting hooks.

The expanded single-PR scope also includes MinIO wire-contract bootstrap validation, durable site-replication retry, full add-time IAM/bootstrap sync, lifecycle compatibility, and site-level repair.

The peer path compatibility step maps outbound peer requests to `/minio/admin/v3/site-replication/...`. Peer join uses the encrypted MinIO payload contract, while internal peer metadata/IAM/remove/edit requests remain plain JSON to match MinIO handlers.
